use crate::sync::native::{Arc, Condvar, Mutex};
use rustix::fs::{AtFlags, Statx};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::ExitStatusExt;

// Lock order: owner state -> either syscall domain. The namespace and flock
// locks never nest or call an owner/ledger. Fixture metadata is immutable before
// actors start; it is not a synchronization dependency between syscalls.
loom::lazy_static! {
    static ref CONTEXT: Arc<Kernel> = Arc::new(Kernel::new());
}

pub(in crate::native_mount) struct Kernel {
    fixture: std::sync::OnceLock<Fixture>,
    state: Mutex<State>,
    locks: Mutex<HashMap<(u64, u64), RawFd>>,
    unlocked: Condvar,
    acquired: Condvar,
}
struct Fixture {
    base: Statx,
    helper_stat: rustix::fs::Stat,
    early_release: bool,
}
struct State {
    fence_acquired: bool,
    mount: Option<u64>,
    pinned: HashMap<RawFd, u64>,
    helper_running: bool,
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
            state: Mutex::new(State {
                fence_acquired: false,
                mount: Some(0),
                pinned: HashMap::new(),
                helper_running: false,
                live_detaches: 0,
                mount_before_exit: 0,
            }),
            locks: Mutex::new(HashMap::new()),
            unlocked: Condvar::new(),
            acquired: Condvar::new(),
        }
    }

    pub(super) fn install(
        base: Statx,
        helper_stat: rustix::fs::Stat,
        early_release: bool,
    ) -> Arc<Self> {
        let kernel = current().unwrap();
        assert!(
            kernel
                .fixture
                .set(Fixture {
                    base,
                    helper_stat,
                    early_release
                })
                .is_ok()
        );
        kernel
    }

    pub(in crate::native_mount) fn early_release(&self) -> bool {
        self.fixture.get().unwrap().early_release
    }

    pub(in crate::native_mount) fn stat(&self, fd: RawFd, flags: AtFlags) -> io::Result<Statx> {
        let mut state = self.state.lock().unwrap();
        let current = state.mount.unwrap_or(2);
        let mount = *state.pinned.entry(fd).or_insert(current);
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

    pub(in crate::native_mount) fn try_lock(&self, file: &File) -> io::Result<()> {
        let identity = Self::identity(file);
        let mut locks = self.locks.lock().unwrap();
        if let Some(owner) = locks.get(&identity) {
            if *owner == file.as_raw_fd() {
                return Ok(());
            }
            return Err(io::Error::from_raw_os_error(libc::EWOULDBLOCK));
        }
        locks.insert(identity, file.as_raw_fd());
        drop(locks);
        // Start the competing opener at the successful fence syscall. Its
        // opposite acquisition order is checked separately, avoiding schedules
        // that differ only in unrelated constructor/accounting instructions.
        let path = std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
        if path.file_name().is_some_and(|name| {
            name.as_encoded_bytes().len() == super::super::NATIVE_FENCE_NAME_BYTES
        }) {
            self.state.lock().unwrap().fence_acquired = true;
            self.acquired.notify_all();
        }
        Ok(())
    }

    pub(super) fn wait_for_recovery_fence(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.fence_acquired {
            state = self.acquired.wait(state).unwrap();
        }
    }

    pub(super) fn wait_for_retirement(&self) {
        let mut locks = self.locks.lock().unwrap();
        while !locks.is_empty() {
            locks = self.unlocked.wait(locks).unwrap();
        }
    }

    pub(in crate::native_mount) fn unlock(&self, file: &File) {
        let identity = Self::identity(file);
        let mut locks = self.locks.lock().unwrap();
        if locks.get(&identity) == Some(&file.as_raw_fd()) {
            locks.remove(&identity);
            self.unlocked.notify_all();
        }
    }

    pub(in crate::native_mount) fn wait_for_unlock(&self, file: &File) {
        let identity = Self::identity(file);
        let mut locks = self.locks.lock().unwrap();
        while locks
            .get(&identity)
            .is_some_and(|fd| *fd != file.as_raw_fd())
        {
            locks = self.unlocked.wait(locks).unwrap();
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
        let (live_detaches, mount_before_exit, helper_running) = {
            let state = self.state.lock().unwrap();
            (
                state.live_detaches,
                state.mount_before_exit,
                state.helper_running,
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
        kernel.state.lock().unwrap().helper_running = true;
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
        // makes the real wait loop retain custody across that interval.
        loom::thread::yield_now();
        if !self.unmounted {
            let mut state = self.kernel.as_ref().unwrap().state.lock().unwrap();
            if state.mount == Some(1) {
                state.live_detaches += 1;
            }
            state.mount = None;
            self.unmounted = true;
            return Ok(None);
        }
        self.kernel
            .as_ref()
            .unwrap()
            .state
            .lock()
            .unwrap()
            .helper_running = false;
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
        self.kernel
            .as_ref()
            .unwrap()
            .state
            .lock()
            .unwrap()
            .helper_running = false;
        self.exited = true;
        Ok(())
    }

    pub(in crate::native_mount) fn is_reaped(&self) -> bool {
        self.real
            .as_ref()
            .map_or(self.exited, |real| real.is_reaped())
    }
}
