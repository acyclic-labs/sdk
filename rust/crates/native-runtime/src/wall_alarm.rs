//! One process-wide alarm on the wall clock.
//!
//! Signed scopes expire at an absolute Unix time, so a monotonic sleep derived
//! once from that time misses a forward clock change. Every registration here
//! shares one index and one thread, which waits on the platform's own
//! wall-clock primitive: a `CLOCK_REALTIME` absolute timerfd that is cancelled
//! on clock changes on Linux, a `NOTE_ABSOLUTE` kqueue timer plus the
//! `com.apple.system.clock_set` notification on Apple platforms, and an
//! absolute waitable timer, which follows system time changes, on Windows.
//! Each wake re-reads the clock and fires every elapsed registration, so a
//! clock change is observed without polling.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

/// Called once, on the alarm thread, when its deadline has passed. It must
/// only signal its owner and return.
pub type AlarmCallback = Arc<dyn Fn() + Send + Sync>;

/// A pending alarm. Dropping it cancels the alarm.
#[must_use = "dropping the registration cancels the alarm"]
pub struct WallAlarm {
    key: (u64, u64),
}

impl WallAlarm {
    /// Calls `fire` once the wall clock reaches `deadline_unix_ms`, at once if
    /// it already has.
    pub fn register(deadline_unix_ms: u64, fire: AlarmCallback) -> io::Result<Self> {
        let alarms = alarms()?;
        let key = locked(&alarms.index).insert(deadline_unix_ms, fire);
        // A failed thread fires everything registered before it set the
        // flag; a later registration must not wait on it.
        if alarms.failed.load(Ordering::SeqCst) {
            locked(&alarms.index).entries.remove(&key);
            return Err(io::Error::other("wall-clock alarm thread failed"));
        }
        let registration = Self { key };
        alarms.waiter.rearm()?;
        Ok(registration)
    }
}

impl Drop for WallAlarm {
    fn drop(&mut self) {
        // The thread may still wake for this deadline; it finds nothing due
        // and waits for the next one.
        if let Ok(alarms) = alarms() {
            locked(&alarms.index).entries.remove(&self.key);
        }
    }
}

#[derive(Default)]
struct Index {
    next: u64,
    entries: BTreeMap<(u64, u64), AlarmCallback>,
}

impl Index {
    fn insert(&mut self, deadline: u64, fire: AlarmCallback) -> (u64, u64) {
        let key = (deadline, self.next);
        self.next = self.next.wrapping_add(1);
        self.entries.insert(key, fire);
        key
    }

    /// Removes and returns the callbacks due at `now`, and the next deadline.
    fn take_due(&mut self, now: u64) -> (Vec<AlarmCallback>, Option<u64>) {
        let pending = self.entries.split_off(&(now.saturating_add(1), 0));
        let due = std::mem::replace(&mut self.entries, pending);
        (
            due.into_values().collect(),
            self.entries.keys().next().map(|(deadline, _)| *deadline),
        )
    }
}

struct Alarms {
    index: Arc<Mutex<Index>>,
    waiter: Arc<platform::Waiter>,
    failed: Arc<AtomicBool>,
}

fn locked(index: &Mutex<Index>) -> MutexGuard<'_, Index> {
    index.lock().unwrap_or_else(PoisonError::into_inner)
}

fn alarms() -> io::Result<&'static Alarms> {
    static ALARMS: OnceLock<Result<Alarms, String>> = OnceLock::new();
    ALARMS
        .get_or_init(|| start().map_err(|error| error.to_string()))
        .as_ref()
        .map_err(|cause| io::Error::other(format!("wall-clock alarm unavailable: {cause}")))
}

fn start() -> io::Result<Alarms> {
    let index = Arc::new(Mutex::new(Index::default()));
    let waiter = Arc::new(platform::Waiter::new()?);
    let failed = Arc::new(AtomicBool::new(false));
    let (driven, waits, failure) = (Arc::clone(&index), Arc::clone(&waiter), Arc::clone(&failed));
    std::thread::Builder::new()
        .name("acyclic-wall-alarm".into())
        .spawn(move || drive(&driven, &*waits, &failure, now_unix_ms))?;
    Ok(Alarms {
        index,
        waiter,
        failed,
    })
}

/// What one wait of the alarm thread does; the platform waiter and the test
/// model implement it.
trait Wait {
    /// Returns once `deadline` has passed on the wall clock, the clock was
    /// changed, or a registration was added.
    fn wait(&self, deadline: Option<u64>) -> io::Result<()>;
}

impl Wait for platform::Waiter {
    fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
        platform::Waiter::wait(self, deadline)
    }
}

fn drive(index: &Mutex<Index>, waiter: &dyn Wait, failed: &AtomicBool, now: impl Fn() -> u64) {
    loop {
        let (due, next) = locked(index).take_due(now());
        for fire in due {
            fire();
        }
        if waiter.wait(next).is_err() {
            // An unusable timer cannot keep any deadline: fire everything so
            // owners fail closed, and refuse later registrations.
            failed.store(true, Ordering::SeqCst);
            let (due, _) = locked(index).take_due(u64::MAX);
            for fire in due {
                fire();
            }
            return;
        }
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(target_os = "linux")]
#[allow(
    unsafe_code,
    reason = "the wall-clock timer and its wake handles are platform OS calls"
)]
mod platform {
    use std::io;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};

    pub(super) struct Waiter {
        timer: OwnedFd,
        rearm: OwnedFd,
    }

    fn owned(fd: libc::c_int) -> io::Result<OwnedFd> {
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful call returned this fresh descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    impl Waiter {
        pub(super) fn new() -> io::Result<Self> {
            // SAFETY: plain descriptor-creating calls without pointers.
            let timer = owned(unsafe {
                libc::timerfd_create(libc::CLOCK_REALTIME, libc::TFD_CLOEXEC | libc::TFD_NONBLOCK)
            })?;
            // SAFETY: as above.
            let rearm = owned(unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) })?;
            Ok(Self { timer, rearm })
        }

        pub(super) fn rearm(&self) -> io::Result<()> {
            let one = 1_u64.to_ne_bytes();
            // SAFETY: writes eight bytes from a live buffer to an owned eventfd.
            let written = unsafe { libc::write(self.rearm.as_raw_fd(), one.as_ptr().cast(), 8) };
            if written < 0 {
                let error = io::Error::last_os_error();
                // A saturated counter is already readable.
                if error.kind() != io::ErrorKind::WouldBlock {
                    return Err(error);
                }
            }
            Ok(())
        }

        pub(super) fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
            let value = deadline.map_or(
                libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                |deadline| libc::timespec {
                    tv_sec: libc::time_t::try_from(deadline / 1000).unwrap_or(libc::time_t::MAX),
                    // Below one second, so it fits every `c_long`.
                    tv_nsec: libc::c_long::try_from((deadline % 1000) * 1_000_000).unwrap_or(0),
                },
            );
            let setting = libc::itimerspec {
                it_interval: libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                it_value: value,
            };
            // An absolute realtime timer fires when the wall clock reaches
            // the deadline; cancel-on-set also wakes on any clock change.
            let flags = if deadline.is_some() {
                libc::TFD_TIMER_ABSTIME | libc::TFD_TIMER_CANCEL_ON_SET
            } else {
                0
            };
            // SAFETY: an owned timerfd and a live setting; no old value is read.
            if unsafe {
                libc::timerfd_settime(
                    self.timer.as_raw_fd(),
                    flags,
                    &raw const setting,
                    std::ptr::null_mut(),
                )
            } < 0
            {
                let error = io::Error::last_os_error();
                // The clock changed while arming: wake and re-read it.
                if error.raw_os_error() == Some(libc::ECANCELED) {
                    return Ok(());
                }
                return Err(error);
            }
            let mut ready = [
                libc::pollfd {
                    fd: self.timer.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: self.rearm.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            // SAFETY: two live pollfd entries for owned descriptors.
            if unsafe { libc::poll(ready.as_mut_ptr(), 2, -1) } < 0 {
                let error = io::Error::last_os_error();
                return if error.kind() == io::ErrorKind::Interrupted {
                    Ok(())
                } else {
                    Err(error)
                };
            }
            for fd in [&self.timer, &self.rearm] {
                drain(fd)?;
            }
            Ok(())
        }
    }

    fn drain(fd: &OwnedFd) -> io::Result<()> {
        let mut count = [0_u8; 8];
        // SAFETY: reads at most eight bytes into a live buffer.
        if unsafe { libc::read(fd.as_raw_fd(), count.as_mut_ptr().cast(), 8) } < 0 {
            let error = io::Error::last_os_error();
            // Not ready, or a timer cancelled by a clock change.
            if error.kind() != io::ErrorKind::WouldBlock
                && error.raw_os_error() != Some(libc::ECANCELED)
            {
                return Err(error);
            }
        }
        Ok(())
    }
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "the wall-clock timer and its wake handles are platform OS calls"
)]
mod platform {
    use std::io;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};

    unsafe extern "C" {
        fn notify_register_file_descriptor(
            name: *const libc::c_char,
            notify_fd: *mut libc::c_int,
            flags: libc::c_int,
            out_token: *mut libc::c_int,
        ) -> u32;
        fn notify_cancel(token: libc::c_int) -> u32;
    }

    const REARM: usize = 1;
    const TIMER: usize = 2;

    pub(super) struct Waiter {
        queue: OwnedFd,
        clock_set: OwnedFd,
        token: libc::c_int,
    }

    fn event(ident: usize, filter: i16, flags: u16, fflags: u32, data: isize) -> libc::kevent {
        libc::kevent {
            ident,
            filter,
            flags,
            fflags,
            data,
            udata: std::ptr::null_mut(),
        }
    }

    impl Waiter {
        pub(super) fn new() -> io::Result<Self> {
            // SAFETY: creates one fresh kqueue descriptor.
            let queue = unsafe { libc::kqueue() };
            if queue < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: a successful call returned this fresh descriptor.
            let queue = unsafe { OwnedFd::from_raw_fd(queue) };
            let (mut fd, mut token) = (-1, 0);
            // SAFETY: a NUL-terminated name and two live out-parameters.
            let status = unsafe {
                notify_register_file_descriptor(
                    c"com.apple.system.clock_set".as_ptr(),
                    &raw mut fd,
                    0,
                    &raw mut token,
                )
            };
            if status != 0 || fd < 0 {
                return Err(io::Error::other(format!(
                    "clock-change notification registration failed: {status}"
                )));
            }
            // SAFETY: the notification system transferred this descriptor.
            let clock_set = unsafe { OwnedFd::from_raw_fd(fd) };
            let waiter = Self {
                queue,
                clock_set,
                token,
            };
            // SAFETY: an owned descriptor; only its status flags change.
            if unsafe {
                libc::fcntl(
                    waiter.clock_set.as_raw_fd(),
                    libc::F_SETFL,
                    libc::O_NONBLOCK,
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
            waiter.change(&[
                event(
                    REARM,
                    libc::EVFILT_USER,
                    libc::EV_ADD | libc::EV_CLEAR,
                    0,
                    0,
                ),
                event(
                    usize::try_from(waiter.clock_set.as_raw_fd()).unwrap_or(usize::MAX),
                    libc::EVFILT_READ,
                    libc::EV_ADD,
                    0,
                    0,
                ),
            ])?;
            Ok(waiter)
        }

        fn change(&self, changes: &[libc::kevent]) -> io::Result<()> {
            let count = libc::c_int::try_from(changes.len()).unwrap_or(libc::c_int::MAX);
            // SAFETY: a live change list and no event list on an owned kqueue.
            if unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    changes.as_ptr(),
                    count,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub(super) fn rearm(&self) -> io::Result<()> {
            self.change(&[event(REARM, libc::EVFILT_USER, 0, libc::NOTE_TRIGGER, 0)])
        }

        pub(super) fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
            match deadline {
                // An absolute calendar timer; the clock-set notification
                // covers a clock change made after it was armed.
                Some(deadline) => self.change(&[event(
                    TIMER,
                    libc::EVFILT_TIMER,
                    libc::EV_ADD | libc::EV_ONESHOT,
                    libc::NOTE_ABSOLUTE | libc::NOTE_USECONDS,
                    isize::try_from(deadline.saturating_mul(1000)).unwrap_or(isize::MAX),
                )])?,
                None => {
                    // Absent when it already fired or was never armed.
                    let _ = self.change(&[event(TIMER, libc::EVFILT_TIMER, libc::EV_DELETE, 0, 0)]);
                }
            }
            let mut ready = [event(0, 0, 0, 0, 0)];
            // SAFETY: one live event slot on an owned kqueue; no timeout.
            if unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    ready.as_mut_ptr(),
                    1,
                    std::ptr::null(),
                )
            } < 0
            {
                let error = io::Error::last_os_error();
                return if error.kind() == io::ErrorKind::Interrupted {
                    Ok(())
                } else {
                    Err(error)
                };
            }
            let mut tokens = [0_u8; 64];
            // SAFETY: drains posted tokens into a live buffer.
            while unsafe {
                libc::read(
                    self.clock_set.as_raw_fd(),
                    tokens.as_mut_ptr().cast(),
                    tokens.len(),
                )
            } > 0
            {}
            Ok(())
        }
    }

    impl Drop for Waiter {
        fn drop(&mut self) {
            // SAFETY: cancels this waiter's own registration once.
            unsafe { notify_cancel(self.token) };
        }
    }
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "the wall-clock timer and its wake handles are platform OS calls"
)]
mod platform {
    use std::io;
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
    use windows_sys::Win32::Foundation::{HANDLE, WAIT_FAILED};
    use windows_sys::Win32::System::Threading::{
        CancelWaitableTimer, CreateEventW, CreateWaitableTimerExW, INFINITE,
        SYNCHRONIZATION_SYNCHRONIZE, SetEvent, SetWaitableTimerEx, TIMER_MODIFY_STATE,
        WaitForMultipleObjects,
    };

    /// 100-nanosecond intervals from 1601 to the Unix epoch.
    const UNIX_EPOCH_TICKS: i64 = 116_444_736_000_000_000;

    pub(super) struct Waiter {
        timer: OwnedHandle,
        rearm: OwnedHandle,
    }

    fn owned(handle: HANDLE) -> io::Result<OwnedHandle> {
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful creation transfers one fresh owned handle.
        Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
    }

    impl Waiter {
        pub(super) fn new() -> io::Result<Self> {
            // SAFETY: an unnamed timer with default security.
            let timer = owned(unsafe {
                CreateWaitableTimerExW(
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                    SYNCHRONIZATION_SYNCHRONIZE | TIMER_MODIFY_STATE,
                )
            })?;
            // SAFETY: an unnamed, initially clear, auto-reset event.
            let rearm = owned(unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) })?;
            Ok(Self { timer, rearm })
        }

        pub(super) fn rearm(&self) -> io::Result<()> {
            // SAFETY: signals an owned event.
            if unsafe { SetEvent(self.rearm.as_raw_handle()) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub(super) fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
            if let Some(deadline) = deadline {
                // A positive due time is absolute UTC; the system adjusts
                // absolute timers when its time changes.
                let due = i64::try_from(deadline)
                    .ok()
                    .and_then(|deadline| deadline.checked_mul(10_000))
                    .and_then(|ticks| ticks.checked_add(UNIX_EPOCH_TICKS))
                    .unwrap_or(i64::MAX);
                // SAFETY: an owned timer and a live due time, without a callback.
                if unsafe {
                    SetWaitableTimerEx(
                        self.timer.as_raw_handle(),
                        &raw const due,
                        0,
                        None,
                        std::ptr::null(),
                        std::ptr::null(),
                        0,
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
            } else {
                // SAFETY: an owned timer; cancelling an unarmed timer is a no-op.
                unsafe { CancelWaitableTimer(self.timer.as_raw_handle()) };
            }
            let handles = [self.timer.as_raw_handle(), self.rearm.as_raw_handle()];
            // SAFETY: two owned handles that outlive the wait.
            if unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, INFINITE) } == WAIT_FAILED {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::sync::mpsc;

    fn counter() -> (Arc<AtomicU64>, AlarmCallback) {
        let fired = Arc::new(AtomicU64::new(0));
        let count = Arc::clone(&fired);
        (
            fired,
            Arc::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
            }),
        )
    }

    #[test]
    fn signed_scope_expiry_wakes_a_quiet_owner_without_a_task_deadline() -> io::Result<()> {
        // Nothing else registers or changes after this alarm: only the
        // platform's wall-clock wait can deliver it.
        let (sender, received) = mpsc::channel();
        let sender = Mutex::new(sender);
        let _alarm = WallAlarm::register(
            now_unix_ms() + 50,
            Arc::new(move || {
                let _ = locked_sender(&sender).send(());
            }),
        )?;
        received
            .recv_timeout(std::time::Duration::from_secs(30))
            .map_err(|_| io::Error::other("quiet alarm never fired"))
    }

    fn locked_sender<T>(sender: &Mutex<T>) -> MutexGuard<'_, T> {
        sender.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[test]
    fn an_elapsed_deadline_fires_at_registration_and_a_dropped_one_never() -> io::Result<()> {
        let (sender, received) = mpsc::channel();
        let sender = Mutex::new(sender);
        let _elapsed = WallAlarm::register(
            0,
            Arc::new(move || {
                let _ = locked_sender(&sender).send(());
            }),
        )?;
        received
            .recv_timeout(std::time::Duration::from_secs(30))
            .map_err(|_| io::Error::other("elapsed alarm never fired"))?;
        let (fired, fire) = counter();
        drop(WallAlarm::register(now_unix_ms() + 20, fire)?);
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(fired.load(Ordering::SeqCst), 0, "a cancelled alarm fired");
        Ok(())
    }

    /// The thread's own loop over a scripted clock: each wait advances the
    /// clock as the platform's wake would report it.
    struct Script {
        clock: Arc<AtomicU64>,
        steps: Mutex<Vec<(Option<u64>, u64)>>,
    }

    impl Wait for Script {
        fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
            let mut steps = locked_sender(&self.steps);
            if steps.is_empty() {
                return Err(io::Error::other("script finished"));
            }
            let (armed, now) = steps.remove(0);
            assert_eq!(deadline, armed, "the alarm armed another deadline");
            self.clock.store(now, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn deadline_registration_rearms_after_a_clock_jump() {
        let index = Mutex::new(Index::default());
        let (fired, fire) = counter();
        locked(&index).insert(10_000, Arc::clone(&fire));
        locked(&index).insert(60_000, fire);
        let clock = Arc::new(AtomicU64::new(0));
        // The first wait arms 10s; the clock is then set forward past it
        // before that monotonic span could elapse. The wake re-reads the
        // clock, fires the elapsed alarm and re-arms the later one, which a
        // second forward change also fires.
        let script = Script {
            clock: Arc::clone(&clock),
            steps: Mutex::new(vec![
                (Some(10_000), 30_000),
                (Some(60_000), 90_000),
                (None, 90_000),
            ]),
        };
        let now = Arc::clone(&clock);
        // The finished script ends the loop through its failure path.
        let failed = AtomicBool::new(false);
        drive(&index, &script, &failed, move || now.load(Ordering::SeqCst));
        assert_eq!(fired.load(Ordering::SeqCst), 2);
        assert!(locked(&index).entries.is_empty());
    }
}
