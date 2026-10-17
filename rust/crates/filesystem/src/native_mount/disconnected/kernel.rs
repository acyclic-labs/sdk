use crate::sync::{Arc, Condvar, Mutex};
use rustix::fs::{AtFlags, Statx};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::ExitStatusExt;

// Lock order: owner state -> work ledger -> kernel state. The global context
// lock is released before entering the kernel; no kernel callback enters an
// owner or work ledger. Registry waits release the kernel lock atomically.
loom::lazy_static! {
    static ref CONTEXT: Mutex<Option<Arc<Kernel>>> = Mutex::new(None);
}

pub(in crate::native_mount) struct Kernel {
    base: Statx,
    helper_stat: rustix::fs::Stat,
    pub(in crate::native_mount) early_release: bool,
    state: Mutex<State>,
    unlocked: Condvar,
}
struct State {
    mount: Option<u64>,
    pinned: HashMap<RawFd, u64>,
    locks: HashMap<(u64, u64), RawFd>,
    helper_running: bool,
    live_detaches: usize,
    mount_before_exit: usize,
}

pub(in crate::native_mount) fn current() -> Option<Arc<Kernel>> {
    CONTEXT.lock().unwrap().clone()
}

impl Kernel {
    pub(super) fn install(
        base: Statx,
        helper_stat: rustix::fs::Stat,
        early_release: bool,
    ) -> Arc<Self> {
        let kernel = Arc::new(Self {
            base,
            helper_stat,
            early_release,
            state: Mutex::new(State {
                mount: Some(0),
                pinned: HashMap::new(),
                locks: HashMap::new(),
                helper_running: false,
                live_detaches: 0,
                mount_before_exit: 0,
            }),
            unlocked: Condvar::new(),
        });
        *CONTEXT.lock().unwrap() = Some(kernel.clone());
        kernel
    }

    pub(in crate::native_mount) fn stat(&self, fd: RawFd, flags: AtFlags) -> io::Result<Statx> {
        let mut state = self.state.lock().unwrap();
        let current = state.mount.unwrap_or(2);
        let mount = *state.pinned.entry(fd).or_insert(current);
        if flags.contains(AtFlags::STATX_FORCE_SYNC) && mount == 0 {
            return Err(io::Error::from_raw_os_error(libc::ENOTCONN));
        }
        let mut stat = self.base;
        stat.stx_mnt_id = mount;
        Ok(stat)
    }

    pub(in crate::native_mount) fn current_stat(&self) -> Statx {
        let mut stat = self.base;
        stat.stx_mnt_id = self.state.lock().unwrap().mount.unwrap_or(2);
        stat
    }

    fn identity(file: &File) -> (u64, u64) {
        let metadata = file.metadata().unwrap();
        (metadata.dev(), metadata.ino())
    }

    pub(in crate::native_mount) fn try_lock(&self, file: &File) -> io::Result<()> {
        let identity = Self::identity(file);
        let mut state = self.state.lock().unwrap();
        if let Some(owner) = state.locks.get(&identity) {
            if *owner == file.as_raw_fd() {
                return Ok(());
            }
            return Err(io::Error::from_raw_os_error(libc::EWOULDBLOCK));
        }
        state.locks.insert(identity, file.as_raw_fd());
        Ok(())
    }

    pub(in crate::native_mount) fn unlock(&self, file: &File) {
        let identity = Self::identity(file);
        let mut state = self.state.lock().unwrap();
        if state.locks.get(&identity) == Some(&file.as_raw_fd()) {
            state.locks.remove(&identity);
            self.unlocked.notify_all();
        }
    }

    pub(in crate::native_mount) fn wait_for_unlock(&self, file: &File) {
        let identity = Self::identity(file);
        let mut state = self.state.lock().unwrap();
        while state
            .locks
            .get(&identity)
            .is_some_and(|fd| *fd != file.as_raw_fd())
        {
            state = self.unlocked.wait(state).unwrap();
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
        let state = self.state.lock().unwrap();
        assert_eq!(
            state.live_detaches, 0,
            "helper detached a live remount after early fence release"
        );
        assert_eq!(
            state.mount_before_exit, 0,
            "remount acquired the fence before helper exit"
        );
        assert!(!state.helper_running, "helper custody was abandoned");
    }

    pub(in crate::native_mount) fn helper_stat(&self) -> rustix::fs::Stat {
        self.helper_stat
    }
}

// An OS-process handle at the existing closed-process syscall boundary. The
// production FuseRecoveryControl still owns spawn, wait, terminate and custody.
pub(in crate::native_mount) struct Helper {
    kernel: Arc<Kernel>,
    exited: bool,
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
            kernel,
            exited: false,
        });
        Ok(())
    }

    pub(in crate::native_mount) fn try_wait(
        &mut self,
        _meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> io::Result<Option<std::process::ExitStatus>> {
        // Explore helper execution against a competing real fence acquirer.
        loom::thread::yield_now();
        if !self.exited {
            let mut state = self.kernel.state.lock().unwrap();
            if state.mount == Some(1) {
                state.live_detaches += 1;
            }
            state.mount = None;
            state.helper_running = false;
            self.exited = true;
        }
        Ok(Some(std::process::ExitStatus::from_raw(0)))
    }

    pub(in crate::native_mount) fn terminate(
        &mut self,
        _meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> io::Result<()> {
        self.kernel.state.lock().unwrap().helper_running = false;
        self.exited = true;
        Ok(())
    }
}
